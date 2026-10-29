FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src
COPY Directory.Build.props Directory.Packages.props ./
COPY apps/shared/OrderFlow.Shared.csproj apps/shared/
COPY apps/api/OrderFlow.Api.csproj apps/api/
RUN dotnet restore apps/api/OrderFlow.Api.csproj
COPY apps/shared/ apps/shared/
COPY apps/api/ apps/api/
RUN dotnet publish apps/api/OrderFlow.Api.csproj -c Release -o /app --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0-alpine
WORKDIR /app
RUN apk add --no-cache krb5-libs
ENV ASPNETCORE_HTTP_PORTS=3000 \
    DOTNET_EnableDiagnostics=1
COPY --from=build /app ./
USER $APP_UID
EXPOSE 3000
ENTRYPOINT ["dotnet", "OrderFlow.Api.dll"]
